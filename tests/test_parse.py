"""Tests for :mod:`davasus._parse`."""

from __future__ import annotations

from datetime import datetime

from davasus._parse import (
    FENCE_DIST_SENTINELS,
    normalise_timestamp,
    parse_fence_distance,
    parse_float,
    parse_int,
)


def test_normalise_timestamp_short_offset():
    """Campbell weather format normalises to ISO-8601 with ``+HH:MM``."""
    out = normalise_timestamp("2024-02-21 00:00:00+00")
    assert out == "2024-02-21T00:00:00+00:00"
    # And it round-trips through fromisoformat in Python 3.10+.
    assert datetime.fromisoformat(out).year == 2024


def test_normalise_timestamp_full_offset():
    """Merged-file timestamps are already ``+HH:MM``."""
    out = normalise_timestamp("2024-03-01 01:00:00+00:00")
    assert out == "2024-03-01T01:00:00+00:00"


def test_normalise_timestamp_empty():
    """Empty / whitespace cells return ``None``."""
    assert normalise_timestamp("") is None
    assert normalise_timestamp("   ") is None


def test_parse_float_handles_blanks_and_na():
    """Empty and NA-style cells map to ``None``."""
    assert parse_float("") is None
    assert parse_float(" NA ") is None
    assert parse_float("nan") is None
    assert parse_float("12.5") == 12.5


def test_parse_int_via_float():
    """``parse_int`` accepts ``"3.0"``-style integers."""
    assert parse_int("3") == 3
    assert parse_int("3.0") == 3
    assert parse_int("") is None


def test_parse_fence_distance_strips_sentinel():
    """Both eShepherd no-fence sentinels become ``None``."""
    assert parse_fence_distance("-2147483647.0") is None
    assert parse_fence_distance("-2147483648.0") is None
    assert parse_fence_distance("-12.5") == -12.5
    assert -2147483647.0 in FENCE_DIST_SENTINELS
