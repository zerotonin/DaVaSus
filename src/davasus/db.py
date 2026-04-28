"""Database connection helpers for DaVaSus.

Wraps :mod:`sqlite3` with a small :class:`Database` class that knows how
to open the database, apply ingest-time PRAGMAs, install the schema, and
upsert rows into the dimension tables (`animals`, `neckbands`,
`source_files`).

All long-running ingestion code talks to SQLite through this class so we
have one place to tune connection settings.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from davasus.schema import Schema

# ────────────────────────────────────────────────────────────────────────
#  « Folder label for files that live at the dataset root »
# ────────────────────────────────────────────────────────────────────────

STANDALONE_FOLDER = "(standalone)"


class Database:
    """Open a DaVaSus SQLite database and serve dimension-table helpers.

    The class is intentionally thin: it owns the :class:`sqlite3.Connection`,
    exposes the underlying schema operations, and provides
    ``get_or_create_*`` methods for the small dimension tables. Fact-table
    inserts are issued by the ingestor classes directly.

    Attributes:
        path: Filesystem path of the SQLite file.
        connection: Open SQLite connection.
        schema: :class:`Schema` instance bound to ``connection``.
    """

    def __init__(self, path: str | Path) -> None:
        """Open (or create) the SQLite database at ``path``.

        Args:
            path: Filesystem path to the SQLite file. Parent directory
                must already exist.
        """
        self.path = Path(path)
        self.connection = sqlite3.connect(self.path)
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.schema = Schema(self.connection)

    # ── lifecycle ──────────────────────────────────────────────────────

    def initialise(self) -> None:
        """Apply ingest PRAGMAs and create all tables (idempotent)."""
        self.schema.apply_pragmas()
        self.schema.create_all()

    def finalise(self) -> None:
        """Build post-ingest indices and commit."""
        self.schema.create_indices()
        self.connection.commit()

    def close(self) -> None:
        """Commit and close the underlying connection."""
        self.connection.commit()
        self.connection.close()

    def __enter__(self) -> Database:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.close()
        else:
            self.connection.close()

    # ── dimension-table helpers ────────────────────────────────────────

    def register_source_file(self, path: str | Path, folder: str | None = None) -> int:
        """Insert (or fetch) a row in ``source_files`` and return its id.

        Args:
            path: Path of the source CSV. The basename is stored.
            folder: Containing folder label. If ``None``, defaults to
                ``"(standalone)"``.

        Returns:
            The ``file_id`` of the registered row.
        """
        p = Path(path)
        filename = p.name
        folder = folder if folder is not None else STANDALONE_FOLDER
        cur = self.connection.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO source_files (filename, folder) VALUES (?, ?)",
            (filename, folder),
        )
        cur.execute(
            "SELECT file_id FROM source_files WHERE filename = ? AND folder = ?",
            (filename, folder),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"Failed to register source file: {filename!r}")
        return int(row[0])

    def upsert_animal(self, animal_id: int) -> None:
        """Insert ``animal_id`` into ``animals`` if not already present.

        Args:
            animal_id: EU ear tag (or local identifier) integer.
        """
        self.connection.execute(
            "INSERT OR IGNORE INTO animals (animal_id) VALUES (?)",
            (int(animal_id),),
        )

    def upsert_animals(self, animal_ids: set[int] | list[int]) -> None:
        """Bulk-insert a collection of animal ids.

        Args:
            animal_ids: Iterable of integer ids; duplicates are silently ignored.
        """
        rows = [(int(a),) for a in animal_ids]
        self.connection.executemany(
            "INSERT OR IGNORE INTO animals (animal_id) VALUES (?)",
            rows,
        )

    def upsert_neckband(self, neckband_id: str) -> None:
        """Insert ``neckband_id`` into ``neckbands`` if not already present.

        Args:
            neckband_id: eShepherd device identifier (string).
        """
        self.connection.execute(
            "INSERT OR IGNORE INTO neckbands (neckband_id) VALUES (?)",
            (neckband_id,),
        )

    def upsert_neckbands(self, neckband_ids: set[str] | list[str]) -> None:
        """Bulk-insert a collection of neckband ids.

        Args:
            neckband_ids: Iterable of string ids; duplicates are silently ignored.
        """
        rows = [(n,) for n in neckband_ids]
        self.connection.executemany(
            "INSERT OR IGNORE INTO neckbands (neckband_id) VALUES (?)",
            rows,
        )

    # ── convenience ────────────────────────────────────────────────────

    def count_rows(self, table: str) -> int:
        """Return the row count of ``table``.

        Args:
            table: Table name. Caller must validate against injection;
                only used internally with hard-coded names.

        Returns:
            Number of rows.
        """
        cur = self.connection.execute(f"SELECT COUNT(*) FROM {table}")
        return int(cur.fetchone()[0])
