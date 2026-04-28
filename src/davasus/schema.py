"""SQLite schema for the DaVaSus database.

The database follows a star schema: small dimension tables hold entity
identity (animals, neckbands, source files) and large fact tables hold
time-series sensor measurements with foreign keys back to the dimensions.

This module is the single source of truth for the schema. The :class:`Schema`
class exposes :meth:`Schema.create_all` to initialise all tables on a
connection, and :meth:`Schema.create_indices` to build the post-ingest
indices once bulk loading is complete.

Design notes:
    * Timestamps are stored as ISO-8601 ``TEXT``. SQLite's date/time
      functions operate natively on this representation and timezone-aware
      strings round-trip through ``datetime.fromisoformat``.
    * Columns are snake_case. The CSV-header → column-name mapping lives
      in the per-source ``COLUMN_MAP`` constants in :mod:`davasus.ingest_*`.
    * The fence-distance sentinel ``-2147483647`` (and ``-2147483648``) is
      stripped at ingest time, so the stored values are real distances or
      ``NULL``.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable

# ────────────────────────────────────────────────────────────────────────
#  « PRAGMA settings for fast bulk insertion »
# ────────────────────────────────────────────────────────────────────────
#
#  Applied at connection time. ``synchronous = OFF`` and ``journal_mode =
#  MEMORY`` are unsafe for concurrent writers but acceptable for a single
#  ingestion process — if it crashes we re-run from the source CSVs.

INGEST_PRAGMAS: tuple[str, ...] = (
    "PRAGMA journal_mode = MEMORY",
    "PRAGMA synchronous = OFF",
    "PRAGMA temp_store = MEMORY",
    "PRAGMA cache_size = -200000",  # 200 MB page cache
    "PRAGMA foreign_keys = ON",
)


# ────────────────────────────────────────────────────────────────────────
#  « Dimension tables »
# ────────────────────────────────────────────────────────────────────────

DIM_TABLES: tuple[tuple[str, str], ...] = (
    (
        "animals",
        """
        CREATE TABLE IF NOT EXISTS animals (
            animal_id        INTEGER PRIMARY KEY,
            sex              TEXT,
            origin_country   TEXT,
            calving_first    TEXT,
            has_bolus        INTEGER
        )
        """,
    ),
    (
        "neckbands",
        """
        CREATE TABLE IF NOT EXISTS neckbands (
            neckband_id      TEXT PRIMARY KEY
        )
        """,
    ),
    (
        "source_files",
        """
        CREATE TABLE IF NOT EXISTS source_files (
            file_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            filename         TEXT NOT NULL,
            folder           TEXT NOT NULL,
            UNIQUE (filename, folder)
        )
        """,
    ),
)


# ────────────────────────────────────────────────────────────────────────
#  « Fact tables »
# ────────────────────────────────────────────────────────────────────────

FACT_TABLES: tuple[tuple[str, str], ...] = (
    (
        "eshepherd",
        """
        CREATE TABLE IF NOT EXISTS eshepherd (
            neckband_id        TEXT    NOT NULL REFERENCES neckbands(neckband_id),
            animal_id          INTEGER          REFERENCES animals(animal_id),
            timestamp          TEXT    NOT NULL,
            gnss_lat           REAL,
            gnss_lon           REAL,
            odometer_km        REAL,
            audio_stim_count   REAL,
            pulse_stim_count   REAL,
            fence_dist_max     REAL,
            fence_dist_min     REAL,
            imu_tick_40mg      INTEGER,
            imu_tick_80mg      INTEGER,
            imu_tick_120mg     INTEGER,
            imu_tick_160mg     INTEGER,
            imu_tick_200mg     INTEGER,
            imu_tick_240mg     INTEGER,
            file_id            INTEGER NOT NULL REFERENCES source_files(file_id)
        )
        """,
    ),
    (
        "smaxtec",
        """
        CREATE TABLE IF NOT EXISTS smaxtec (
            animal_id                   INTEGER NOT NULL REFERENCES animals(animal_id),
            timestamp                   TEXT    NOT NULL,
            act_index                   REAL,
            temp                        REAL,
            temp_normal_index           REAL,
            heat_index                  REAL,
            calving_index               REAL,
            rum_index_x                 REAL,
            rum_index_y                 REAL,
            act                         REAL,
            temp_dec_index              REAL,
            temp_height_index           REAL,
            temp_inc_index              REAL,
            temp_without_drink_cycles   REAL,
            water_intake                REAL,
            file_id                     INTEGER NOT NULL REFERENCES source_files(file_id)
        )
        """,
    ),
    (
        "weather",
        """
        CREATE TABLE IF NOT EXISTS weather (
            timestamp              TEXT PRIMARY KEY,
            record                 INTEGER,
            air_temp_min           REAL,
            air_temp_avg           REAL,
            air_temp_max           REAL,
            air_temp_std           REAL,
            rel_humid_min          REAL,
            rel_humid_avg          REAL,
            rel_humid_max          REAL,
            rain_mm_tot            REAL,
            rain_corr_mm_tot       REAL,
            bp_mbar_min            REAL,
            bp_mbar_avg            REAL,
            bp_mbar_max            REAL,
            bp_mbar_std            REAL,
            wind_dir_deg           REAL,
            wind_spd_min           REAL,
            wind_spd_avg           REAL,
            wind_spd_max           REAL,
            wind_spd_std           REAL,
            tdewpt_c_avg           REAL,
            twetbulb_c_avg         REAL,
            sun_hrs_tot            REAL,
            pot_slr_rad_avg        REAL,
            ground_temp_min        REAL,
            ground_temp_avg        REAL,
            ground_temp_max        REAL,
            ground_temp_std        REAL,
            rad_swin_min           REAL,
            rad_swin_avg           REAL,
            rad_swin_max           REAL,
            rad_swin_std           REAL,
            rad_swout_min          REAL,
            rad_swout_avg          REAL,
            rad_swout_max          REAL,
            rad_swout_std          REAL,
            rad_lwin_min           REAL,
            rad_lwin_avg           REAL,
            rad_lwin_max           REAL,
            rad_lwin_std           REAL,
            rad_lwout_min          REAL,
            rad_lwout_avg          REAL,
            rad_lwout_max          REAL,
            rad_lwout_std          REAL,
            rad_swnet_min          REAL,
            rad_swnet_avg          REAL,
            rad_swnet_max          REAL,
            rad_swnet_std          REAL,
            rad_lwnet_min          REAL,
            rad_lwnet_avg          REAL,
            rad_lwnet_max          REAL,
            rad_lwnet_std          REAL,
            rad_sw_albedo_min      REAL,
            rad_sw_albedo_avg      REAL,
            rad_sw_albedo_max      REAL,
            rad_sw_albedo_std      REAL,
            rad_net_min            REAL,
            rad_net_avg            REAL,
            rad_net_max            REAL,
            rad_net_std            REAL,
            shf_a_avg              REAL,
            shf_b_avg              REAL,
            vwc_c_avg              REAL,
            vwc_d_avg              REAL,
            file_id                INTEGER NOT NULL REFERENCES source_files(file_id)
        )
        """,
    ),
)


# ────────────────────────────────────────────────────────────────────────
#  « Indices »
# ────────────────────────────────────────────────────────────────────────
#
#  Created after bulk insertion to avoid write-time overhead.

INDICES: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_eshepherd_neckband_ts ON eshepherd(neckband_id, timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_eshepherd_animal_ts   ON eshepherd(animal_id, timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_smaxtec_animal_ts     ON smaxtec(animal_id, timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_source_folder         ON source_files(folder)",
)


class Schema:
    """Apply the DaVaSus schema to a SQLite connection.

    Attributes:
        connection: Open :class:`sqlite3.Connection` to operate on.
    """

    def __init__(self, connection: sqlite3.Connection) -> None:
        """Initialise with an open connection.

        Args:
            connection: SQLite connection. The schema is applied to this
                connection's database via DDL. Caller controls commit.
        """
        self.connection = connection

    def apply_pragmas(self, statements: Iterable[str] = INGEST_PRAGMAS) -> None:
        """Apply ingest-time PRAGMA tuning.

        Args:
            statements: PRAGMA statements; defaults to :data:`INGEST_PRAGMAS`.
        """
        cur = self.connection.cursor()
        for stmt in statements:
            cur.execute(stmt)

    def create_all(self) -> None:
        """Create every dimension and fact table (idempotent)."""
        cur = self.connection.cursor()
        for _name, ddl in DIM_TABLES:
            cur.execute(ddl)
        for _name, ddl in FACT_TABLES:
            cur.execute(ddl)
        self.connection.commit()

    def create_indices(self) -> None:
        """Create post-ingest indices.

        Call once after bulk insertion is complete.
        """
        cur = self.connection.cursor()
        for stmt in INDICES:
            cur.execute(stmt)
        self.connection.commit()

    def table_names(self) -> list[str]:
        """List user tables present in the database.

        Returns:
            Table names in creation order.
        """
        cur = self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY rowid"
        )
        return [row[0] for row in cur.fetchall()]
