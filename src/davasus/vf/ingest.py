# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.ingest                                             ║
# ║  « stream the heifer CSV into heifer_vf.db »                     ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Chunked read, column mapping, sentinel stripping and the split  ║
# ║  into heifers, parcel_events, heifer_fixes and vf_weather.       ║
# ║  Raw values are kept; only the no-fence sentinel becomes NULL.   ║
# ║  Plausibility gates belong to the analysis layer.                ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Ingest the heifer virtual-fence CSV into SQLite."""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Callable, Iterator
from pathlib import Path

import numpy as np
import pandas as pd

from davasus.schema import INGEST_PRAGMAS
from davasus.vf.constants import NO_FENCE_SENTINELS
from davasus.vf.events import PARCEL_LOG_COLUMNS, TIME_COLUMN, ParcelEventBuilder
from davasus.vf.schema import VFSchema

log = logging.getLogger(__name__)

CHUNK_ROWS: int = 200_000
LABEL_COLUMN = "eShepherd parcel activation"

# ┌────────────────────────────────────────────────────────────┐
# │ Column maps  « CSV header to schema column »               │
# └────────────────────────────────────────────────────────────┘

FIX_COLUMN_MAP: dict[str, str] = {
    "Neckband_Number": "neckband_id",
    "LOM_X": "lom",
    "Unix_Timestamp": "timestamp",
    "GNSS_Latitude": "gnss_lat",
    "GNSS_Longitude": "gnss_lon",
    "Odometer_km": "odometer_km",
    "Audio_Stimulus_Count": "audio_stim_count",
    "Pulse_Stimulus_Count": "pulse_stim_count",
    "Distance_To_Fence_Max": "fence_dist_max",
    "Distance_To_Fence_Min": "fence_dist_min",
    "IMU_Tick_Count_40mG": "imu_tick_40mg",
    "IMU_Tick_Count_80mG": "imu_tick_80mg",
    "IMU_Tick_Count_120mG": "imu_tick_120mg",
    "IMU_Tick_Count_160mG": "imu_tick_160mg",
    "IMU_Tick_Count_200mG": "imu_tick_200mg",
    "IMU_Tick_Count_240mG": "imu_tick_240mg",
    "Odometer_km_Distinctive": "odometer_km_new",
    "Audio_Stimulus_Distinctive": "audio_stim_new",
    "Pulse_Stimulus_Distinctive": "pulse_stim_new",
    "In_Permanent_Shadow": "in_shadow",
    "Cluster_Group": "cluster_group",
    "Audio_Stimulus_Sum": "audio_stim_sum",
    "Pulse_Stimulus_Sum": "pulse_stim_sum",
    "Outside": "outside",
}

WEATHER_COLUMN_MAP: dict[str, str] = {
    "TIMESTAMP": "timestamp",
    "AirT_C_Avg": "air_temp_avg",
    "RelHumid": "rel_humid_avg",
    "Rain_mm_Tot": "rain_mm_tot",
    "BP_mbar_Avg": "bp_mbar_avg",
    "WindSpd_m_s_Avg": "wind_spd_avg",
    "Rad_Net_Avg": "rad_net_avg",
    "SunHrs_Tot": "sun_hrs_tot",
}

HEIFER_COLUMN_MAP: dict[str, str] = {
    "LOM_X": "lom",
    "Ohrmarke": "ear_tag",
    "GEB_DATR": "birth_date",
    "GESCHL_R": "sex",
    "RASSE": "breed",
    "GVE": "gve",
}

INTEGER_FIX_COLUMNS: tuple[str, ...] = (
    "imu_tick_40mg", "imu_tick_80mg", "imu_tick_120mg", "imu_tick_160mg",
    "imu_tick_200mg", "imu_tick_240mg", "in_shadow", "cluster_group", "outside",
)
FENCE_DISTANCE_COLUMNS: tuple[str, ...] = ("fence_dist_max", "fence_dist_min")

REQUIRED_COLUMNS: tuple[str, ...] = tuple(
    dict.fromkeys(
        (*FIX_COLUMN_MAP, *WEATHER_COLUMN_MAP, *HEIFER_COLUMN_MAP, *PARCEL_LOG_COLUMNS)
    )
)

ProgressCallback = Callable[[int], None]


class HeaderError(ValueError):
    """Raised when the CSV lacks columns the ingestor needs."""


# ┌────────────────────────────────────────────────────────────┐
# │ Ingestor  « two passes: metadata, then fixes »             │
# └────────────────────────────────────────────────────────────┘


class HeiferVFIngestor:
    """Load one heifer CSV into an empty ``heifer_vf.db``.

    Pass 1 reads the parcel-log and register columns to build the event,
    exclusion, heifer and neckband tables. Pass 2 streams the fixes and
    weather in chunks. A fix that appears twice at a label boundary keeps
    the row of the later activation (higher ``event_id``).
    """

    def __init__(
        self,
        connection: sqlite3.Connection,
        csv_path: Path,
        chunk_rows: int = CHUNK_ROWS,
    ) -> None:
        self.connection = connection
        self.csv_path = csv_path
        self.chunk_rows = chunk_rows
        self.schema = VFSchema(connection)

    def run(
        self,
        test_n: int | None = None,
        progress: ProgressCallback | None = None,
    ) -> dict[str, int]:
        """Ingest the whole file, or its first ``test_n`` rows.

        Args:
            test_n:   Read only this many data rows (smoke tests).
            progress: Called with the number of rows after each chunk.

        Returns:
            Row counts per table plus ``rows_read`` and ``duplicate_fixes``.
        """
        self._validate_header()
        for pragma in INGEST_PRAGMAS:
            self.connection.execute(pragma)
        self.schema.create_all()
        self._refuse_non_empty()

        label_to_event = self._ingest_metadata(test_n)
        rows_read = self._ingest_fixes(label_to_event, test_n, progress)
        self.schema.create_indices()

        counts = self.table_counts()
        counts["rows_read"] = rows_read
        counts["duplicate_fixes"] = rows_read - counts["heifer_fixes"]
        log.info("heifer ingest: %s", counts)
        return counts

    def table_counts(self) -> dict[str, int]:
        names = (
            "heifers", "vf_neckbands", "parcel_events", "parcel_event_exclusions",
            "heifer_fixes", "vf_weather",
        )
        return {
            name: self.connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            for name in names
        }

    # ─────────────────────────────────────────────────────────────
    #  Pass 1: events, exclusions, heifers, neckbands
    # ─────────────────────────────────────────────────────────────

    def _ingest_metadata(self, test_n: int | None) -> dict[str, int]:
        meta_columns = list(
            dict.fromkeys((TIME_COLUMN, "Neckband_Number", *HEIFER_COLUMN_MAP, *PARCEL_LOG_COLUMNS))
        )
        meta = pd.read_csv(
            self.csv_path, usecols=meta_columns, nrows=test_n,
            dtype={"Neckband_Number": "string"}, low_memory=False,
        )
        builder = ParcelEventBuilder()
        events = builder.build(meta)
        exclusions = builder.build_exclusions(meta, events)
        neckbands = set(meta["Neckband_Number"].dropna()) | set(exclusions["neckband_id"])

        self._insert("vf_neckbands", pd.DataFrame({"neckband_id": sorted(neckbands)}))
        self._insert("heifers", build_heifer_table(meta))
        self._insert("parcel_events", events)
        self._insert("parcel_event_exclusions", exclusions)
        self.connection.commit()
        return dict(zip(events["label"], events["event_id"], strict=True))

    # ─────────────────────────────────────────────────────────────
    #  Pass 2: fixes and weather, chunked
    # ─────────────────────────────────────────────────────────────

    def _ingest_fixes(
        self,
        label_to_event: dict[str, int],
        test_n: int | None,
        progress: ProgressCallback | None,
    ) -> int:
        fix_sql = build_fix_upsert_sql()
        weather_sql = build_insert_sql("vf_weather", list(WEATHER_COLUMN_MAP.values()), "IGNORE")
        rows_read = 0
        for chunk in self._iter_chunks(test_n):
            fixes = transform_fixes(chunk, label_to_event)
            weather = transform_weather(chunk)
            self.connection.executemany(fix_sql, _records(fixes))
            self.connection.executemany(weather_sql, _records(weather))
            self.connection.commit()
            rows_read += len(chunk)
            if progress is not None:
                progress(len(chunk))
        return rows_read

    def _iter_chunks(self, test_n: int | None) -> Iterator[pd.DataFrame]:
        columns = [*FIX_COLUMN_MAP, *WEATHER_COLUMN_MAP, LABEL_COLUMN]
        yield from pd.read_csv(
            self.csv_path, usecols=list(dict.fromkeys(columns)), nrows=test_n,
            chunksize=self.chunk_rows, dtype={"Neckband_Number": "string"},
            low_memory=False,
        )

    # ─────────────────────────────────────────────────────────────
    #  Helpers
    # ─────────────────────────────────────────────────────────────

    def _validate_header(self) -> None:
        header = pd.read_csv(self.csv_path, nrows=0).columns
        missing = [c for c in REQUIRED_COLUMNS if c not in header]
        if missing:
            raise HeaderError(f"{self.csv_path.name} lacks columns: {', '.join(missing)}")

    def _refuse_non_empty(self) -> None:
        if self.connection.execute("SELECT COUNT(*) FROM heifer_fixes").fetchone()[0]:
            raise RuntimeError("heifer_fixes is not empty; ingest into a fresh database")

    def _insert(self, table: str, frame: pd.DataFrame) -> None:
        sql = build_insert_sql(table, list(frame.columns))
        self.connection.executemany(sql, _records(frame))


# ┌────────────────────────────────────────────────────────────┐
# │ Transforms  « pure functions, unit-tested »                │
# └────────────────────────────────────────────────────────────┘


def build_heifer_table(meta: pd.DataFrame) -> pd.DataFrame:
    """One row per heifer: ISO birth date, decimal-point GVE."""
    heifers = (
        meta[list(HEIFER_COLUMN_MAP)]
        .drop_duplicates("LOM_X")
        .rename(columns=HEIFER_COLUMN_MAP)
        .reset_index(drop=True)
    )
    heifers["birth_date"] = pd.to_datetime(
        heifers["birth_date"], format="%d/%m/%Y"
    ).dt.strftime("%Y-%m-%d")
    heifers["gve"] = pd.to_numeric(
        heifers["gve"].astype("string").str.replace(",", ".", regex=False)
    )
    return heifers


def transform_fixes(chunk: pd.DataFrame, label_to_event: dict[str, int]) -> pd.DataFrame:
    """Map one CSV chunk onto ``heifer_fixes`` columns."""
    fixes = chunk[list(FIX_COLUMN_MAP)].rename(columns=FIX_COLUMN_MAP)
    fixes["timestamp"] = normalise_timestamps(fixes["timestamp"])
    fixes["event_id"] = chunk[LABEL_COLUMN].map(label_to_event).astype("Int64")
    for column in FENCE_DISTANCE_COLUMNS:
        fixes[column] = fixes[column].where(~fixes[column].isin(NO_FENCE_SENTINELS))
    for column in INTEGER_FIX_COLUMNS:
        fixes[column] = fixes[column].round().astype("Int64")
    return fixes


def transform_weather(chunk: pd.DataFrame) -> pd.DataFrame:
    """Distinct weather records of one chunk."""
    weather = chunk[list(WEATHER_COLUMN_MAP)].rename(columns=WEATHER_COLUMN_MAP)
    weather = weather.drop_duplicates("timestamp")
    weather["timestamp"] = normalise_timestamps(weather["timestamp"])
    return weather


def normalise_timestamps(values: pd.Series) -> pd.Series:
    """``2024-07-25 18:26:00+00:00`` to ``2024-07-25T18:26:00+00:00``."""
    stamps = pd.to_datetime(values, utc=True)
    return pd.Series(
        np.char.add(stamps.dt.strftime("%Y-%m-%dT%H:%M:%S").to_numpy(dtype=str), "+00:00"),
        index=values.index,
    )


def build_insert_sql(table: str, columns: list[str], conflict: str = "ABORT") -> str:
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT OR {conflict} INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"


def build_fix_upsert_sql() -> str:
    """Insert a fix; on a duplicate key keep the later activation's row."""
    columns = [*FIX_COLUMN_MAP.values(), "event_id"]
    updates = ", ".join(
        f"{c} = excluded.{c}" for c in columns if c not in ("neckband_id", "timestamp")
    )
    placeholders = ", ".join("?" for _ in columns)
    return (
        f"INSERT INTO heifer_fixes ({', '.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT (neckband_id, timestamp) DO UPDATE SET {updates} "
        "WHERE excluded.event_id > heifer_fixes.event_id"
    )


def _records(frame: pd.DataFrame) -> list[tuple[object, ...]]:
    clean = frame.astype(object).where(frame.notna(), None)
    return list(clean.itertuples(index=False, name=None))
