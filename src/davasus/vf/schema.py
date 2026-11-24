# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.schema                                             ║
# ║  « normalise the denormalised heifer CSV »                       ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Star schema for heifer_vf.db. The CSV repeats weather, parcel   ║
# ║  log and HIT register on every fix; here each lives once.        ║
# ║  Precomputed THI, Date, Time_Diff and per-day sums are not       ║
# ║  stored: they are derived at analysis time (D10).                ║
# ╚══════════════════════════════════════════════════════════════════╝
"""SQLite schema for the heifer virtual-fence database."""

from __future__ import annotations

import sqlite3

# ┌────────────────────────────────────────────────────────────┐
# │ Dimension tables  « animals, collars, parcel events »      │
# └────────────────────────────────────────────────────────────┘

DIM_TABLES: tuple[tuple[str, str], ...] = (
    (
        "heifers",
        """
        CREATE TABLE IF NOT EXISTS heifers (
            lom          TEXT PRIMARY KEY,
            ear_tag      INTEGER,
            birth_date   TEXT,
            sex          TEXT,
            breed        TEXT,
            gve          REAL
        )
        """,
    ),
    (
        "vf_neckbands",
        """
        CREATE TABLE IF NOT EXISTS vf_neckbands (
            neckband_id  TEXT PRIMARY KEY
        )
        """,
    ),
    (
        "parcel_events",
        """
        CREATE TABLE IF NOT EXISTS parcel_events (
            event_id               INTEGER PRIMARY KEY,
            label                  TEXT NOT NULL,
            open_parcels           TEXT NOT NULL,
            cut_off_parcel         INTEGER,
            new_parcel             INTEGER,
            is_transition          INTEGER NOT NULL DEFAULT 0,
            start_utc              TEXT NOT NULL,
            end_utc                TEXT,
            parcel_size_ha         REAL,
            vf_length_m            REAL,
            total_time_min         REAL,
            water_point_relocated  INTEGER,
            manual_herding         INTEGER,
            biomass_mm             REAL,
            biomass_kg_ha          REAL,
            comments               TEXT
        )
        """,
    ),
    (
        "parcel_event_exclusions",
        """
        CREATE TABLE IF NOT EXISTS parcel_event_exclusions (
            event_id     INTEGER NOT NULL REFERENCES parcel_events(event_id),
            neckband_id  TEXT    NOT NULL REFERENCES vf_neckbands(neckband_id),
            reason       TEXT    NOT NULL,
            PRIMARY KEY (event_id, neckband_id, reason)
        )
        """,
    ),
)

# ┌────────────────────────────────────────────────────────────┐
# │ Fact tables  « one row per collar fix »                    │
# └────────────────────────────────────────────────────────────┘

FACT_TABLES: tuple[tuple[str, str], ...] = (
    (
        "heifer_fixes",
        """
        CREATE TABLE IF NOT EXISTS heifer_fixes (
            neckband_id        TEXT    NOT NULL REFERENCES vf_neckbands(neckband_id),
            lom                TEXT    NOT NULL REFERENCES heifers(lom),
            event_id           INTEGER          REFERENCES parcel_events(event_id),
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
            odometer_km_new    REAL,
            audio_stim_new     REAL,
            pulse_stim_new     REAL,
            in_shadow          INTEGER,
            cluster_group      INTEGER,
            audio_stim_sum     REAL,
            pulse_stim_sum     REAL,
            outside            INTEGER,
            PRIMARY KEY (neckband_id, timestamp)
        ) WITHOUT ROWID
        """,
    ),
    (
        "vf_weather",
        """
        CREATE TABLE IF NOT EXISTS vf_weather (
            timestamp       TEXT PRIMARY KEY,
            air_temp_avg    REAL,
            rel_humid_avg   REAL,
            rain_mm_tot     REAL,
            bp_mbar_avg     REAL,
            wind_spd_avg    REAL,
            rad_net_avg     REAL,
            sun_hrs_tot     REAL
        )
        """,
    ),
    (
        "space_weather",
        """
        CREATE TABLE IF NOT EXISTS space_weather (
            timestamp   TEXT PRIMARY KEY,
            hp30        REAL,
            ap30        REAL,
            kp          REAL,
            roti_farm   REAL
        )
        """,
    ),
)

# The primary key (neckband_id, timestamp) on a WITHOUT ROWID table is the
# clustered index for per-collar time series. Secondary indices cover the
# other access paths:
#   ix_fixes_ts       herd snapshots: every collar at one minute
#   ix_fixes_lom_ts   per-animal series by ear tag
#   ix_fixes_event    per-event phases, e.g. Outside == 3 after activation
#   ix_fixes_cluster  stimulus clusters (numbered per collar)
INDICES: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS ix_fixes_ts ON heifer_fixes (timestamp)",
    "CREATE INDEX IF NOT EXISTS ix_fixes_lom_ts ON heifer_fixes (lom, timestamp)",
    "CREATE INDEX IF NOT EXISTS ix_fixes_event ON heifer_fixes (event_id, outside)",
    "CREATE INDEX IF NOT EXISTS ix_fixes_cluster ON heifer_fixes (neckband_id, cluster_group)",
)


class VFSchema:
    """Create and index the heifer virtual-fence tables on a connection."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def create_all(self) -> None:
        for _, ddl in (*DIM_TABLES, *FACT_TABLES):
            self.connection.execute(ddl)
        self.connection.commit()

    def create_indices(self) -> None:
        """Build secondary indices and planner statistics after bulk ingest."""
        for ddl in INDICES:
            self.connection.execute(ddl)
        self.connection.execute("ANALYZE")
        self.connection.commit()

    def table_names(self) -> list[str]:
        cur = self.connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        return [row[0] for row in cur.fetchall()]
