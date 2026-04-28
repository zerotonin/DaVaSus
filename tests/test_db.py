"""Tests for :mod:`davasus.db`."""

from __future__ import annotations

from davasus.db import Database


def test_initialise_creates_tables(database: Database):
    """`Database.initialise()` produces the full table set."""
    names = set(database.schema.table_names())
    assert {"animals", "neckbands", "source_files",
            "eshepherd", "smaxtec", "weather"}.issubset(names)


def test_register_source_file_is_idempotent(database: Database, tmp_path):
    """Re-registering the same file returns the same id."""
    p = tmp_path / "foo.csv"
    p.write_text("ignored")
    a = database.register_source_file(p)
    b = database.register_source_file(p)
    assert a == b


def test_upsert_animals_dedupes(database: Database):
    """`upsert_animals` writes each id at most once."""
    database.upsert_animals({1, 2, 3})
    database.upsert_animals({2, 3, 4})
    assert database.count_rows("animals") == 4


def test_upsert_neckbands_dedupes(database: Database):
    """`upsert_neckbands` writes each id at most once."""
    database.upsert_neckbands({"a", "b"})
    database.upsert_neckbands({"b", "c"})
    assert database.count_rows("neckbands") == 3
