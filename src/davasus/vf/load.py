# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.load                                               ║
# ║  « query heifer_vf.db into tidy frames »                         ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Thin readers for fixes, events and the heifer register with     ║
# ║  UTC-aware timestamps. Filtering by animal and time runs in      ║
# ║  SQL so the indices do the work.                                 ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Read tables from ``heifer_vf.db`` into pandas."""

from __future__ import annotations

import sqlite3

import pandas as pd


def load_fixes(
    connection: sqlite3.Connection,
    lom: str | None = None,
    start_utc: str | None = None,
    end_utc: str | None = None,
) -> pd.DataFrame:
    """Fixes, optionally for one animal and a half-open UTC window.

    Args:
        connection: Open ``heifer_vf.db`` connection.
        lom:        Ear tag (``heifers.lom``); all animals when ``None``.
        start_utc:  Inclusive lower bound, ISO-8601.
        end_utc:    Exclusive upper bound, ISO-8601.
    """
    clauses: list[str] = []
    params: list[str] = []
    if lom is not None:
        clauses.append("lom = ?")
        params.append(lom)
    if start_utc is not None:
        clauses.append("timestamp >= ?")
        params.append(_iso(start_utc))
    if end_utc is not None:
        clauses.append("timestamp < ?")
        params.append(_iso(end_utc))
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"SELECT * FROM heifer_fixes{where} ORDER BY neckband_id, timestamp"
    fixes = pd.read_sql_query(sql, connection, params=params)
    fixes["timestamp"] = pd.to_datetime(fixes["timestamp"], utc=True, format="ISO8601")
    return fixes


def load_events(connection: sqlite3.Connection) -> pd.DataFrame:
    events = pd.read_sql_query("SELECT * FROM parcel_events ORDER BY event_id", connection)
    for column in ("start_utc", "end_utc"):
        events[column] = pd.to_datetime(events[column], utc=True, format="ISO8601")
    return events


def load_space_weather(connection: sqlite3.Connection) -> pd.DataFrame:
    """GFZ indices (``davasus-vf-spaceweather``); empty if never fetched."""
    frame = pd.read_sql_query("SELECT * FROM space_weather ORDER BY timestamp", connection)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, format="ISO8601")
    return frame


def load_parcel_geometry(connection: sqlite3.Connection) -> dict[str, pd.DataFrame]:
    """Parcel outlines, paddocks, open paddocks and tree patches (``davasus-vf-paddocks``).

    Empty frames when the tables are missing or were never filled.
    """
    queries = {
        "parcel_outlines": "SELECT * FROM parcel_outlines ORDER BY label, vertex",
        "paddocks": "SELECT * FROM paddocks ORDER BY parzelle, ring, vertex",
        "parcel_paddocks": "SELECT * FROM parcel_paddocks ORDER BY label, paddock",
        "tree_patches": "SELECT * FROM tree_patches ORDER BY patch, ring, vertex",
    }
    frames = {}
    for name, sql in queries.items():
        try:
            frames[name] = pd.read_sql_query(sql, connection)
        except pd.errors.DatabaseError:
            frames[name] = pd.DataFrame()
    return frames


def load_heifers(connection: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query("SELECT * FROM heifers ORDER BY lom", connection)


def find_most_stimulated_heifer(connection: sqlite3.Connection) -> str:
    """Ear tag with the most audio cues; a sensible default demo animal."""
    row = connection.execute(
        "SELECT lom FROM heifer_fixes GROUP BY lom ORDER BY SUM(audio_stim_new) DESC LIMIT 1"
    ).fetchone()
    if row is None:
        raise ValueError("heifer_fixes is empty")
    return str(row[0])


def _iso(value: str) -> str:
    """Normalise a user-supplied time to the stored ISO-8601 UTC form."""
    return pd.Timestamp(value, tz="UTC").strftime("%Y-%m-%dT%H:%M:%S+00:00")
