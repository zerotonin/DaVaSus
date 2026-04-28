"""Tests for :mod:`davasus.schema`."""

from __future__ import annotations

import sqlite3

from davasus.schema import DIM_TABLES, FACT_TABLES, Schema


def test_create_all_creates_every_table(tmp_path):
    """`Schema.create_all()` creates every listed dim and fact table."""
    con = sqlite3.connect(tmp_path / "x.db")
    try:
        schema = Schema(con)
        schema.apply_pragmas()
        schema.create_all()
        present = set(schema.table_names())
        expected = {name for name, _ in DIM_TABLES} | {name for name, _ in FACT_TABLES}
        assert expected.issubset(present)
    finally:
        con.close()


def test_create_indices_is_idempotent(tmp_path):
    """Calling ``create_indices`` twice does not fail."""
    con = sqlite3.connect(tmp_path / "x.db")
    try:
        schema = Schema(con)
        schema.create_all()
        schema.create_indices()
        schema.create_indices()
    finally:
        con.close()
