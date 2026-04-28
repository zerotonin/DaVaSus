"""Stream the raw ATB weather-station CSV into the ``weather`` fact table.

The Campbell Scientific export has ~120 columns; we keep the curated
subset declared in :data:`WEATHER_COLUMN_MAP` and ignore sensor
housekeeping (battery voltage, panel temperature, diagnostic counters,
power-on totals).

Usage:
    >>> from davasus.db import Database
    >>> db = Database("cow.db"); db.initialise()
    >>> WeatherIngestor(db).ingest("Weather_2024.csv")
    >>> db.finalise(); db.close()
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from davasus._parse import normalise_timestamp, parse_float, parse_int
from davasus.db import Database

log = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────
#  « CSV-header → DB-column map »
# ────────────────────────────────────────────────────────────────────────
#
#  Order matches the ``weather`` schema in :mod:`davasus.schema`. Columns
#  not listed here are intentionally dropped.

WEATHER_COLUMN_MAP: dict[str, str] = {
    "TIMESTAMP":          "timestamp",
    "RECORD":             "record",
    "AirT_C_Min":         "air_temp_min",
    "AirT_C_Avg":         "air_temp_avg",
    "AirT_C_Max":         "air_temp_max",
    "AirT_C_Std":         "air_temp_std",
    "RelHumid_Min":       "rel_humid_min",
    "RelHumid":           "rel_humid_avg",
    "RelHumid_Max":       "rel_humid_max",
    "Rain_mm_Tot":        "rain_mm_tot",
    "Rain_corr_mm_Tot":   "rain_corr_mm_tot",
    "BP_mbar_Min":        "bp_mbar_min",
    "BP_mbar_Avg":        "bp_mbar_avg",
    "BP_mbar_Max":        "bp_mbar_max",
    "BP_mbar_Std":        "bp_mbar_std",
    "WindDir_deg":        "wind_dir_deg",
    "WindSpd_m_s_Min":    "wind_spd_min",
    "WindSpd_m_s_Avg":    "wind_spd_avg",
    "WindSpd_m_s_Max":    "wind_spd_max",
    "WindSpd_m_s_Std":    "wind_spd_std",
    "Tdewpt_C_Avg":       "tdewpt_c_avg",
    "Twetbulb_C_Avg":     "twetbulb_c_avg",
    "SunHrs_Tot":         "sun_hrs_tot",
    "PotSlrRad_Avg":      "pot_slr_rad_avg",
    "GroundT_C_Min":      "ground_temp_min",
    "GroundT_C_Avg":      "ground_temp_avg",
    "GroundT_C_Max":      "ground_temp_max",
    "GroundT_C_Std":      "ground_temp_std",
    "Rad_SWin_Min":       "rad_swin_min",
    "Rad_SWin_Avg":       "rad_swin_avg",
    "Rad_SWin_Max":       "rad_swin_max",
    "Rad_SWin_Std":       "rad_swin_std",
    "Rad_SWout_Min":      "rad_swout_min",
    "Rad_SWout_Avg":      "rad_swout_avg",
    "Rad_SWout_Max":      "rad_swout_max",
    "Rad_SWout_Std":      "rad_swout_std",
    "Rad_LWin_Min":       "rad_lwin_min",
    "Rad_LWin_Avg":       "rad_lwin_avg",
    "Rad_LWin_Max":       "rad_lwin_max",
    "Rad_LWin_Std":       "rad_lwin_std",
    "Rad_LWout_Min":      "rad_lwout_min",
    "Rad_LWout_Avg":      "rad_lwout_avg",
    "Rad_LWout_Max":      "rad_lwout_max",
    "Rad_LWout_Std":      "rad_lwout_std",
    "Rad_SWnet_Min":      "rad_swnet_min",
    "Rad_SWnet_Avg":      "rad_swnet_avg",
    "Rad_SWnet_Max":      "rad_swnet_max",
    "Rad_SWnet_Std":      "rad_swnet_std",
    "Rad_LWnet_Min":      "rad_lwnet_min",
    "Rad_LWnet_Avg":      "rad_lwnet_avg",
    "Rad_LWnet_Max":      "rad_lwnet_max",
    "Rad_LWnet_Std":      "rad_lwnet_std",
    "Rad_SWalbedo_Min":   "rad_sw_albedo_min",
    "Rad_SWalbedo_Avg":   "rad_sw_albedo_avg",
    "Rad_SWalbedo_Max":   "rad_sw_albedo_max",
    "Rad_SWalbedo_Std":   "rad_sw_albedo_std",
    "Rad_Net_Min":        "rad_net_min",
    "Rad_Net_Avg":        "rad_net_avg",
    "Rad_Net_Max":        "rad_net_max",
    "Rad_Net_Std":        "rad_net_std",
    "SHF_A_Avg":          "shf_a_avg",
    "SHF_B_Avg":          "shf_b_avg",
    "VWC_C_Avg":          "vwc_c_avg",
    "VWC_D_Avg":          "vwc_d_avg",
}

# Subset of mapped column names that are integer-typed in the schema.
INT_COLUMNS: frozenset[str] = frozenset({"record"})


class WeatherIngestor:
    """Stream a raw weather CSV into the ``weather`` fact table.

    Attributes:
        db: Open :class:`Database` to insert into.
        chunk_size: Number of rows committed per transaction batch.
    """

    def __init__(self, db: Database, chunk_size: int = 5_000) -> None:
        """Bind the ingestor to a database.

        Args:
            db: Database with schema already created (call
                :meth:`Database.initialise` first).
            chunk_size: Rows per ``executemany`` / ``commit`` batch.
        """
        self.db = db
        self.chunk_size = chunk_size

    # ── public API ──────────────────────────────────────────────────────

    def ingest(self, csv_path: str | Path, test_n: int | None = None) -> int:
        """Ingest a single weather CSV.

        Args:
            csv_path: Path to the raw weather export.
            test_n: If set, stop after this many data rows. Useful for
                smoke tests.

        Returns:
            Number of rows actually inserted (after row-level validation).
        """
        path = Path(csv_path)
        file_id = self.db.register_source_file(path)
        log.info("Ingesting weather file %s (file_id=%d)", path.name, file_id)

        insert_sql, db_columns = self._build_insert_sql()
        with path.open("r", newline="") as fh:
            reader = csv.DictReader(fh)
            self._validate_header(reader.fieldnames or [])
            return self._stream_rows(reader, insert_sql, db_columns, file_id, test_n)

    # ── helpers ─────────────────────────────────────────────────────────

    def _validate_header(self, header: list[str]) -> None:
        """Confirm every mapped CSV column appears in the file header.

        Args:
            header: Field names from the CSV header row.

        Raises:
            ValueError: If any expected column is missing.
        """
        missing = [c for c in WEATHER_COLUMN_MAP if c not in header]
        if missing:
            raise ValueError(
                f"Weather CSV is missing expected columns: {missing[:5]}"
                + (f" (… +{len(missing) - 5} more)" if len(missing) > 5 else "")
            )

    def _build_insert_sql(self) -> tuple[str, list[str]]:
        """Build the parameterised INSERT statement for the ``weather`` table.

        Returns:
            ``(sql, db_columns)`` where ``db_columns`` is the column-name
            list in the same order as the ``?`` placeholders.
        """
        db_columns = [*WEATHER_COLUMN_MAP.values(), "file_id"]
        cols_sql = ", ".join(db_columns)
        placeholders = ", ".join("?" for _ in db_columns)
        sql = f"INSERT OR IGNORE INTO weather ({cols_sql}) VALUES ({placeholders})"
        return sql, db_columns

    def _stream_rows(
        self,
        reader: csv.DictReader,
        insert_sql: str,
        db_columns: list[str],
        file_id: int,
        test_n: int | None,
    ) -> int:
        """Read rows in batches and bulk-insert each batch.

        Args:
            reader: DictReader positioned just past the header.
            insert_sql: Parameterised INSERT for the ``weather`` table.
            db_columns: Column-name order matching the ``?`` placeholders.
            file_id: Source-file id to attach to every row.
            test_n: Optional row cap for smoke tests.

        Returns:
            Number of rows inserted.
        """
        cur = self.db.connection.cursor()
        batch: list[tuple] = []
        inserted = 0
        for i, row in enumerate(reader):
            if test_n is not None and i >= test_n:
                break
            parsed = self._parse_row(row, file_id)
            if parsed is None:
                continue
            batch.append(parsed)
            if len(batch) >= self.chunk_size:
                cur.executemany(insert_sql, batch)
                self.db.connection.commit()
                inserted += len(batch)
                batch.clear()
        if batch:
            cur.executemany(insert_sql, batch)
            self.db.connection.commit()
            inserted += len(batch)
        log.info("  inserted %d weather rows", inserted)
        return inserted

    def _parse_row(self, row: dict[str, str], file_id: int) -> tuple | None:
        """Convert a CSV row to a tuple matching :meth:`_build_insert_sql`.

        Args:
            row: DictReader row (CSV column → string value).
            file_id: Source-file id to append.

        Returns:
            Tuple of values, or ``None`` if the timestamp is missing.
        """
        ts = normalise_timestamp(row.get("TIMESTAMP", ""))
        if ts is None:
            return None
        values: list[object] = [ts]
        for csv_col, db_col in list(WEATHER_COLUMN_MAP.items())[1:]:  # skip TIMESTAMP
            raw = row.get(csv_col, "")
            if db_col in INT_COLUMNS:
                values.append(parse_int(raw))
            else:
                values.append(parse_float(raw))
        values.append(file_id)
        return tuple(values)
