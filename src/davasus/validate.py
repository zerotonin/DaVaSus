"""Post-ingest validation for the DaVaSus database.

The :class:`Validator` runs a small battery of checks against the SQLite
database produced by :mod:`davasus.ingest_*` and returns a structured
:class:`ValidationReport`. Reports can be rendered as a plain-text
summary or serialised to JSON for archiving.

The intent is *plausibility*, not statistical inference: row counts,
NULL rates, value ranges, temporal coverage, and referential integrity.
Anything that flags here should prompt a manual look at the source CSV
before downstream analysis runs.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import asdict, dataclass, field
from pathlib import Path

log = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────
#  « Plausibility ranges »
# ────────────────────────────────────────────────────────────────────────
#
#  Per-column physiological / instrument bounds. Values outside these
#  ranges are reported but not deleted — the schema preserves the raw
#  reading and the analysis layer decides what to do with outliers.

VALUE_RANGES: dict[tuple[str, str], tuple[float, float]] = {
    ("smaxtec",   "temp"):                       (30.0, 43.0),
    ("smaxtec",   "temp_without_drink_cycles"):  (30.0, 43.0),
    ("smaxtec",   "act_index"):                  (0.0, 100.0),
    ("smaxtec",   "heat_index"):                 (0.0, 100.0),
    ("eshepherd", "gnss_lat"):                   (-90.0, 90.0),
    ("eshepherd", "gnss_lon"):                   (-180.0, 180.0),
    ("weather",   "air_temp_avg"):               (-40.0, 50.0),
    ("weather",   "rel_humid_avg"):              (0.0, 100.0),
    ("weather",   "wind_spd_avg"):               (0.0, 60.0),
}


# ────────────────────────────────────────────────────────────────────────
#  « Report containers »
# ────────────────────────────────────────────────────────────────────────


@dataclass
class TableSummary:
    """Row count + temporal coverage for a single table.

    Attributes:
        name: Table name.
        rows: Row count.
        ts_min: Earliest timestamp (None for tables without one).
        ts_max: Latest timestamp.
    """

    name: str
    rows: int
    ts_min: str | None = None
    ts_max: str | None = None


@dataclass
class NullReport:
    """NULL fraction for a single ``(table, column)`` cell.

    Attributes:
        table: Table name.
        column: Column name.
        null_rows: Number of NULL values.
        total_rows: Number of rows checked.
        null_rate: Fraction in ``[0, 1]``.
    """

    table: str
    column: str
    null_rows: int
    total_rows: int
    null_rate: float


@dataclass
class RangeReport:
    """Out-of-range fraction for a single ``(table, column)`` cell.

    Attributes:
        table: Table name.
        column: Column name.
        lo: Inclusive lower bound used for the check.
        hi: Inclusive upper bound used for the check.
        out_of_range: Number of non-NULL values outside ``[lo, hi]``.
        non_null_rows: Number of non-NULL values checked.
        out_of_range_rate: Fraction in ``[0, 1]``.
        observed_min: Observed minimum (None if all NULL).
        observed_max: Observed maximum (None if all NULL).
    """

    table: str
    column: str
    lo: float
    hi: float
    out_of_range: int
    non_null_rows: int
    out_of_range_rate: float
    observed_min: float | None
    observed_max: float | None


@dataclass
class ValidationReport:
    """Top-level validation report.

    Attributes:
        db_path: Path of the validated database.
        tables: One :class:`TableSummary` per fact / dim table.
        nulls: One :class:`NullReport` per checked column.
        ranges: One :class:`RangeReport` per range-bounded column.
        orphans: Map of relationship-name → orphan-row count (FK violations).
        warnings: Free-text warnings raised during validation.
    """

    db_path: str
    tables: list[TableSummary] = field(default_factory=list)
    nulls: list[NullReport] = field(default_factory=list)
    ranges: list[RangeReport] = field(default_factory=list)
    orphans: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Return a JSON-serialisable dict.

        Returns:
            Nested dict mirroring the dataclass tree.
        """
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        """Return a JSON string.

        Args:
            indent: Pretty-print indent. Pass ``None`` for compact JSON.

        Returns:
            Serialised report.
        """
        return json.dumps(self.to_dict(), indent=indent)


# ────────────────────────────────────────────────────────────────────────
#  « Validator »
# ────────────────────────────────────────────────────────────────────────


class Validator:
    """Run plausibility checks against a DaVaSus database.

    Attributes:
        connection: Open :class:`sqlite3.Connection`.
        db_path: Path of the database file (for the report header).
    """

    # Tables we expect to exist.
    EXPECTED_DIM_TABLES: tuple[str, ...] = ("animals", "neckbands", "source_files")
    EXPECTED_FACT_TABLES: tuple[str, ...] = ("eshepherd", "smaxtec", "weather")

    # Columns whose NULL rate we report.
    NULL_CHECK_COLUMNS: tuple[tuple[str, str], ...] = (
        ("eshepherd", "animal_id"),
        ("eshepherd", "gnss_lat"),
        ("eshepherd", "fence_dist_min"),
        ("eshepherd", "imu_tick_40mg"),
        ("smaxtec",   "temp"),
        ("smaxtec",   "temp_without_drink_cycles"),
        ("smaxtec",   "rum_index_x"),
        ("weather",   "air_temp_avg"),
        ("weather",   "pot_slr_rad_avg"),
    )

    def __init__(self, connection: sqlite3.Connection, db_path: str | Path) -> None:
        """Bind the validator to an open connection.

        Args:
            connection: SQLite connection in read-only or read-write mode.
            db_path: Path of the database file (used in the report header).
        """
        self.connection = connection
        self.db_path = str(db_path)

    # ── public API ──────────────────────────────────────────────────────

    def run(self) -> ValidationReport:
        """Run every check and return the assembled report.

        Returns:
            Populated :class:`ValidationReport`.
        """
        report = ValidationReport(db_path=self.db_path)
        self._check_tables_exist(report)
        self._summarise_tables(report)
        self._check_nulls(report)
        self._check_ranges(report)
        self._check_orphans(report)
        return report

    # ── individual checks ───────────────────────────────────────────────

    def _check_tables_exist(self, report: ValidationReport) -> None:
        """Add a warning for any missing dim or fact table.

        Args:
            report: Report being assembled.
        """
        cur = self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        present = {row[0] for row in cur.fetchall()}
        for name in self.EXPECTED_DIM_TABLES + self.EXPECTED_FACT_TABLES:
            if name not in present:
                report.warnings.append(f"missing table: {name}")

    def _summarise_tables(self, report: ValidationReport) -> None:
        """Populate row counts and (where applicable) timestamp bounds.

        Args:
            report: Report being assembled.
        """
        for name in self.EXPECTED_DIM_TABLES:
            rows = self._count_rows(name)
            report.tables.append(TableSummary(name=name, rows=rows))
        for name in self.EXPECTED_FACT_TABLES:
            rows = self._count_rows(name)
            ts_min, ts_max = self._timestamp_bounds(name)
            report.tables.append(
                TableSummary(name=name, rows=rows, ts_min=ts_min, ts_max=ts_max)
            )

    def _check_nulls(self, report: ValidationReport) -> None:
        """Compute NULL rates for every entry in :attr:`NULL_CHECK_COLUMNS`.

        Args:
            report: Report being assembled.
        """
        for table, column in self.NULL_CHECK_COLUMNS:
            total = self._count_rows(table)
            if total == 0:
                continue
            cur = self.connection.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL"
            )
            null_rows = int(cur.fetchone()[0])
            report.nulls.append(
                NullReport(
                    table=table,
                    column=column,
                    null_rows=null_rows,
                    total_rows=total,
                    null_rate=null_rows / total,
                )
            )

    def _check_ranges(self, report: ValidationReport) -> None:
        """Compute out-of-range rates for every entry in :data:`VALUE_RANGES`.

        Args:
            report: Report being assembled.
        """
        for (table, column), (lo, hi) in VALUE_RANGES.items():
            cur = self.connection.execute(
                f"SELECT COUNT(*), MIN({column}), MAX({column}) "
                f"FROM {table} WHERE {column} IS NOT NULL"
            )
            non_null, observed_min, observed_max = cur.fetchone()
            non_null = int(non_null)
            if non_null == 0:
                continue
            cur = self.connection.execute(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE {column} IS NOT NULL AND ({column} < ? OR {column} > ?)",
                (lo, hi),
            )
            oor = int(cur.fetchone()[0])
            report.ranges.append(
                RangeReport(
                    table=table,
                    column=column,
                    lo=lo,
                    hi=hi,
                    out_of_range=oor,
                    non_null_rows=non_null,
                    out_of_range_rate=oor / non_null,
                    observed_min=observed_min,
                    observed_max=observed_max,
                )
            )

    def _check_orphans(self, report: ValidationReport) -> None:
        """Count fact rows whose foreign key has no matching dim row.

        Args:
            report: Report being assembled.
        """
        relationships = (
            ("eshepherd.neckband_id → neckbands",
             "SELECT COUNT(*) FROM eshepherd e "
             "LEFT JOIN neckbands n ON n.neckband_id = e.neckband_id "
             "WHERE n.neckband_id IS NULL"),
            ("eshepherd.animal_id → animals",
             "SELECT COUNT(*) FROM eshepherd e "
             "LEFT JOIN animals a ON a.animal_id = e.animal_id "
             "WHERE e.animal_id IS NOT NULL AND a.animal_id IS NULL"),
            ("smaxtec.animal_id → animals",
             "SELECT COUNT(*) FROM smaxtec s "
             "LEFT JOIN animals a ON a.animal_id = s.animal_id "
             "WHERE a.animal_id IS NULL"),
            ("eshepherd.file_id → source_files",
             "SELECT COUNT(*) FROM eshepherd e "
             "LEFT JOIN source_files f ON f.file_id = e.file_id "
             "WHERE f.file_id IS NULL"),
            ("smaxtec.file_id → source_files",
             "SELECT COUNT(*) FROM smaxtec s "
             "LEFT JOIN source_files f ON f.file_id = s.file_id "
             "WHERE f.file_id IS NULL"),
            ("weather.file_id → source_files",
             "SELECT COUNT(*) FROM weather w "
             "LEFT JOIN source_files f ON f.file_id = w.file_id "
             "WHERE f.file_id IS NULL"),
        )
        for label, sql in relationships:
            cur = self.connection.execute(sql)
            report.orphans[label] = int(cur.fetchone()[0])

    # ── tiny helpers ────────────────────────────────────────────────────

    def _count_rows(self, table: str) -> int:
        """Return the row count of ``table``.

        Args:
            table: Table name (validated against ``EXPECTED_*`` lists).

        Returns:
            Row count.
        """
        cur = self.connection.execute(f"SELECT COUNT(*) FROM {table}")
        return int(cur.fetchone()[0])

    def _timestamp_bounds(self, table: str) -> tuple[str | None, str | None]:
        """Return ``(min, max)`` of the ``timestamp`` column.

        Args:
            table: Fact table name.

        Returns:
            Pair of ISO-8601 strings, or ``(None, None)`` if empty.
        """
        cur = self.connection.execute(
            f"SELECT MIN(timestamp), MAX(timestamp) FROM {table}"
        )
        row = cur.fetchone()
        return row[0], row[1]


# ────────────────────────────────────────────────────────────────────────
#  « Plain-text rendering »
# ────────────────────────────────────────────────────────────────────────


def render_report(report: ValidationReport) -> str:
    """Render a :class:`ValidationReport` as a human-readable text block.

    Args:
        report: Report to render.

    Returns:
        Multi-line string suitable for ``print()`` or a CI log.
    """
    lines: list[str] = []
    lines.append("DaVaSus validation report")
    lines.append(f"  database: {report.db_path}")
    lines.append("")

    lines.append("Tables")
    lines.append("------")
    for t in report.tables:
        if t.ts_min is not None:
            lines.append(
                f"  {t.name:14s} rows={t.rows:>14,}   ts=[{t.ts_min} .. {t.ts_max}]"
            )
        else:
            lines.append(f"  {t.name:14s} rows={t.rows:>14,}")
    lines.append("")

    lines.append("NULL rates")
    lines.append("----------")
    for n in report.nulls:
        lines.append(
            f"  {n.table:9s} {n.column:30s} {n.null_rate*100:6.2f}%  "
            f"({n.null_rows:,} / {n.total_rows:,})"
        )
    lines.append("")

    lines.append("Value ranges")
    lines.append("------------")
    for r in report.ranges:
        flag = " ⚠" if r.out_of_range_rate > 0 else ""
        lines.append(
            f"  {r.table:9s} {r.column:30s} "
            f"[{r.lo:g}, {r.hi:g}] -> oor={r.out_of_range_rate*100:5.2f}% "
            f"obs=[{r.observed_min}, {r.observed_max}]{flag}"
        )
    lines.append("")

    lines.append("Referential integrity")
    lines.append("---------------------")
    for label, n in report.orphans.items():
        flag = " ⚠" if n > 0 else " ok"
        lines.append(f"  {label:42s} orphans={n:>10,}{flag}")
    lines.append("")

    if report.warnings:
        lines.append("Warnings")
        lines.append("--------")
        for w in report.warnings:
            lines.append(f"  - {w}")
        lines.append("")

    return "\n".join(lines)
