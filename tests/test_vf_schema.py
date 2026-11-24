"""Tests for :mod:`davasus.vf.schema`."""

from __future__ import annotations

import sqlite3

from davasus.vf.schema import DIM_TABLES, FACT_TABLES, VFSchema


def test_create_all_and_indices(tmp_path):
    con = sqlite3.connect(tmp_path / "vf.db")
    try:
        schema = VFSchema(con)
        schema.create_all()
        schema.create_indices()
        schema.create_indices()
        expected = {name for name, _ in (*DIM_TABLES, *FACT_TABLES)}
        assert expected.issubset(set(schema.table_names()))
    finally:
        con.close()


def test_herd_snapshot_query_uses_an_index(tmp_path):
    con = sqlite3.connect(tmp_path / "vf.db")
    try:
        schema = VFSchema(con)
        schema.create_all()
        schema.create_indices()
        plan = con.execute(
            "EXPLAIN QUERY PLAN SELECT neckband_id FROM heifer_fixes WHERE timestamp = ?",
            ("2024-08-12T12:00:00+00:00",),
        ).fetchall()
        assert any("ix_fixes_ts" in str(row) for row in plan)
    finally:
        con.close()
