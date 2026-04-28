"""Stream the merged eShepherd + smaXtec + weather CSV into the database.

The 2024 merged export from ATB Potsdam denormalises three logically
distinct sources onto every row. This ingestor decomposes each row into:

* one ``eshepherd`` fact row (always written),
* one ``smaxtec`` fact row (skipped when the animal carries no bolus —
  i.e. all bolus columns are empty, which holds for the male-only,
  collar-only animals).

Weather columns in the merged file are intentionally **ignored** here —
they are a truncated subset of the authoritative
``Weather_*.csv`` raw export, which is ingested separately by
:mod:`davasus.ingest_weather`.

Usage:
    >>> from davasus.db import Database
    >>> db = Database("cow.db"); db.initialise()
    >>> MergedIngestor(db).ingest("merged_2024.csv")
    >>> db.finalise(); db.close()
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from davasus._parse import (
    normalise_timestamp,
    parse_fence_distance,
    parse_float,
    parse_int,
)
from davasus.db import Database

if TYPE_CHECKING:
    from collections.abc import Iterator

log = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────
#  « eShepherd column map (CSV → eshepherd fact table) »
# ────────────────────────────────────────────────────────────────────────

ESHEPHERD_COLUMN_MAP: dict[str, str] = {
    "GNSS_Latitude":          "gnss_lat",
    "GNSS_Longitude":         "gnss_lon",
    "Odometer_km":            "odometer_km",
    "Audio_Stimulus_Count":   "audio_stim_count",
    "Pulse_Stimulus_Count":   "pulse_stim_count",
    "Distance_To_Fence_Max":  "fence_dist_max",
    "Distance_To_Fence_Min":  "fence_dist_min",
    "IMU_Tick_Count_40mG":    "imu_tick_40mg",
    "IMU_Tick_Count_80mG":    "imu_tick_80mg",
    "IMU_Tick_Count_120mG":   "imu_tick_120mg",
    "IMU_Tick_Count_160mG":   "imu_tick_160mg",
    "IMU_Tick_Count_200mG":   "imu_tick_200mg",
    "IMU_Tick_Count_240mG":   "imu_tick_240mg",
}

ESHEPHERD_FENCE_COLS: frozenset[str] = frozenset(
    {"Distance_To_Fence_Max", "Distance_To_Fence_Min"}
)

ESHEPHERD_INT_COLS: frozenset[str] = frozenset(
    {f"IMU_Tick_Count_{n}mG" for n in (40, 80, 120, 160, 200, 240)}
)

# ────────────────────────────────────────────────────────────────────────
#  « smaXtec column map (CSV → smaxtec fact table) »
# ────────────────────────────────────────────────────────────────────────

SMAXTEC_COLUMN_MAP: dict[str, str] = {
    "act_index":                  "act_index",
    "temp":                       "temp",
    "temp_normal_index":          "temp_normal_index",
    "heat_index":                 "heat_index",
    "calving_index":              "calving_index",
    "rum_index_x":                "rum_index_x",
    "rum_index_y":                "rum_index_y",
    "act":                        "act",
    "temp_dec_index":             "temp_dec_index",
    "temp_height_index":          "temp_height_index",
    "temp_inc_index":             "temp_inc_index",
    "temp_without_drink_cycles":  "temp_without_drink_cycles",
    "water_intake":               "water_intake",
}


class MergedIngestor:
    """Stream the merged 2024 CSV into ``eshepherd`` + ``smaxtec``.

    Attributes:
        db: Open :class:`Database` to insert into.
        chunk_size: Number of rows per fact-table batch.
    """

    def __init__(self, db: Database, chunk_size: int = 50_000) -> None:
        """Bind the ingestor to a database.

        Args:
            db: Database with schema already created.
            chunk_size: Rows per ``executemany`` / ``commit`` batch.
        """
        self.db = db
        self.chunk_size = chunk_size

    # ── public API ──────────────────────────────────────────────────────

    def ingest(self, csv_path: str | Path, test_n: int | None = None) -> dict[str, int]:
        """Ingest a single merged CSV.

        Args:
            csv_path: Path to the merged-data CSV.
            test_n: If set, stop after this many data rows.

        Returns:
            ``{"eshepherd": n1, "smaxtec": n2}`` row counts.
        """
        path = Path(csv_path)
        file_id = self.db.register_source_file(path)
        log.info("Ingesting merged file %s (file_id=%d)", path.name, file_id)

        eshepherd_sql = self._build_eshepherd_sql()
        smaxtec_sql = self._build_smaxtec_sql()

        with path.open("r", newline="") as fh:
            reader = csv.DictReader(fh)
            self._validate_header(reader.fieldnames or [])
            return self._stream_rows(
                reader, eshepherd_sql, smaxtec_sql, file_id, test_n
            )

    # ── header + SQL builders ───────────────────────────────────────────

    def _validate_header(self, header: list[str]) -> None:
        """Confirm all expected source columns are present.

        Args:
            header: CSV field names.

        Raises:
            ValueError: If any required column is missing.
        """
        required = {"neckband_id", "animal_id", "timestamp"}
        required.update(ESHEPHERD_COLUMN_MAP)
        required.update(SMAXTEC_COLUMN_MAP)
        missing = [c for c in required if c not in header]
        if missing:
            raise ValueError(
                f"Merged CSV is missing expected columns: {missing[:8]}"
                + (f" (… +{len(missing) - 8} more)" if len(missing) > 8 else "")
            )

    def _build_eshepherd_sql(self) -> str:
        """Return the parameterised INSERT for the ``eshepherd`` table."""
        cols = [
            "neckband_id", "animal_id", "timestamp",
            *ESHEPHERD_COLUMN_MAP.values(),
            "file_id",
        ]
        placeholders = ", ".join("?" for _ in cols)
        return f"INSERT INTO eshepherd ({', '.join(cols)}) VALUES ({placeholders})"

    def _build_smaxtec_sql(self) -> str:
        """Return the parameterised INSERT for the ``smaxtec`` table."""
        cols = ["animal_id", "timestamp", *SMAXTEC_COLUMN_MAP.values(), "file_id"]
        placeholders = ", ".join("?" for _ in cols)
        return f"INSERT INTO smaxtec ({', '.join(cols)}) VALUES ({placeholders})"

    # ── row decomposition ───────────────────────────────────────────────

    def _row_to_eshepherd(
        self,
        row: dict[str, str],
        neckband_id: str,
        animal_id: int | None,
        timestamp: str,
        file_id: int,
    ) -> tuple:
        """Build the eshepherd-row tuple for one CSV row.

        Args:
            row: DictReader row.
            neckband_id: Pre-extracted neckband id.
            animal_id: Pre-extracted animal id (may be ``None``).
            timestamp: Normalised timestamp.
            file_id: Source-file id.

        Returns:
            Tuple ready for ``executemany`` against the eshepherd schema.
        """
        values: list[object] = [neckband_id, animal_id, timestamp]
        for csv_col in ESHEPHERD_COLUMN_MAP:
            raw = row.get(csv_col, "")
            if csv_col in ESHEPHERD_FENCE_COLS:
                values.append(parse_fence_distance(raw))
            elif csv_col in ESHEPHERD_INT_COLS:
                values.append(parse_int(raw))
            else:
                values.append(parse_float(raw))
        values.append(file_id)
        return tuple(values)

    def _row_to_smaxtec(
        self,
        row: dict[str, str],
        animal_id: int,
        timestamp: str,
        file_id: int,
    ) -> tuple | None:
        """Build the smaxtec-row tuple, or ``None`` if all bolus values are missing.

        Args:
            row: DictReader row.
            animal_id: Pre-extracted animal id.
            timestamp: Normalised timestamp.
            file_id: Source-file id.

        Returns:
            Tuple ready for ``executemany``, or ``None`` to skip the row.
        """
        parsed = [parse_float(row.get(c, "")) for c in SMAXTEC_COLUMN_MAP]
        if all(v is None for v in parsed):
            return None
        return (animal_id, timestamp, *parsed, file_id)

    # ── streaming loop ──────────────────────────────────────────────────

    def _stream_rows(
        self,
        reader: csv.DictReader,
        eshepherd_sql: str,
        smaxtec_sql: str,
        file_id: int,
        test_n: int | None,
    ) -> dict[str, int]:
        """Read rows in batches and bulk-insert facts; track new dim ids.

        Args:
            reader: DictReader positioned past the header.
            eshepherd_sql: INSERT statement for eshepherd.
            smaxtec_sql: INSERT statement for smaxtec.
            file_id: Source-file id.
            test_n: Optional row cap.

        Returns:
            Row counts per fact table.
        """
        cur = self.db.connection.cursor()
        eshepherd_batch: list[tuple] = []
        smaxtec_batch: list[tuple] = []
        seen_neckbands: set[str] = set()
        seen_animals: set[int] = set()
        new_neckbands: set[str] = set()
        new_animals: set[int] = set()
        eshepherd_n = 0
        smaxtec_n = 0
        for i, row in enumerate(self._iter_rows(reader, test_n)):
            neckband_id = (row.get("neckband_id") or "").strip()
            if not neckband_id:
                continue
            timestamp = normalise_timestamp(row.get("timestamp") or "")
            if timestamp is None:
                continue
            animal_id = parse_int(row.get("animal_id") or "")

            if neckband_id not in seen_neckbands:
                seen_neckbands.add(neckband_id)
                new_neckbands.add(neckband_id)
            if animal_id is not None and animal_id not in seen_animals:
                seen_animals.add(animal_id)
                new_animals.add(animal_id)

            eshepherd_batch.append(
                self._row_to_eshepherd(row, neckband_id, animal_id, timestamp, file_id)
            )
            if animal_id is not None:
                sm_row = self._row_to_smaxtec(row, animal_id, timestamp, file_id)
                if sm_row is not None:
                    smaxtec_batch.append(sm_row)

            if len(eshepherd_batch) >= self.chunk_size:
                self._flush(
                    cur, eshepherd_sql, eshepherd_batch,
                    smaxtec_sql, smaxtec_batch,
                    new_neckbands, new_animals,
                )
                eshepherd_n += len(eshepherd_batch)
                smaxtec_n += len(smaxtec_batch)
                if (i + 1) % (self.chunk_size * 10) == 0:
                    log.info("  processed %d rows", i + 1)
                eshepherd_batch.clear()
                smaxtec_batch.clear()
                new_neckbands.clear()
                new_animals.clear()

        if eshepherd_batch or smaxtec_batch or new_neckbands or new_animals:
            self._flush(
                cur, eshepherd_sql, eshepherd_batch,
                smaxtec_sql, smaxtec_batch,
                new_neckbands, new_animals,
            )
            eshepherd_n += len(eshepherd_batch)
            smaxtec_n += len(smaxtec_batch)

        log.info(
            "  inserted %d eshepherd rows / %d smaxtec rows / %d animals / %d neckbands",
            eshepherd_n, smaxtec_n, len(seen_animals), len(seen_neckbands),
        )
        return {"eshepherd": eshepherd_n, "smaxtec": smaxtec_n}

    def _iter_rows(
        self, reader: csv.DictReader, test_n: int | None
    ) -> Iterator[dict[str, str]]:
        """Yield DictReader rows, capped at ``test_n`` when set.

        Args:
            reader: DictReader to iterate.
            test_n: Optional row cap.

        Yields:
            Row dictionaries.
        """
        if test_n is None:
            yield from reader
        else:
            for i, row in enumerate(reader):
                if i >= test_n:
                    return
                yield row

    def _flush(
        self,
        cur,
        eshepherd_sql: str,
        eshepherd_batch: list[tuple],
        smaxtec_sql: str,
        smaxtec_batch: list[tuple],
        new_neckbands: set[str],
        new_animals: set[int],
    ) -> None:
        """Upsert newly-seen dim rows, executemany the fact batches, commit.

        Args:
            cur: Active cursor.
            eshepherd_sql: INSERT for eshepherd.
            eshepherd_batch: Rows to insert.
            smaxtec_sql: INSERT for smaxtec.
            smaxtec_batch: Rows to insert.
            new_neckbands: Neckband ids first seen during this chunk.
            new_animals: Animal ids first seen during this chunk.
        """
        if new_neckbands:
            self.db.upsert_neckbands(new_neckbands)
        if new_animals:
            self.db.upsert_animals(new_animals)
        if eshepherd_batch:
            cur.executemany(eshepherd_sql, eshepherd_batch)
        if smaxtec_batch:
            cur.executemany(smaxtec_sql, smaxtec_batch)
        self.db.connection.commit()
