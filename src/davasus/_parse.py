"""Shared CSV value parsers for the ingestors.

These helpers turn raw CSV strings into the Python types expected by the
SQLite schema. They are deliberately small and free-standing so the
ingestor classes can import them without circularity.
"""

from __future__ import annotations

import re

# ────────────────────────────────────────────────────────────────────────
#  « Timestamp normalisation »
# ────────────────────────────────────────────────────────────────────────
#
#  The raw CSVs contain timestamps in two flavours:
#      "2024-02-21 00:00:00+00"      (Campbell weather export)
#      "2024-03-01 01:00:00+00:00"   (merged eShepherd / smaXtec export)
#
#  Normalised form:
#      "2024-02-21T00:00:00+00:00"   ISO-8601, parseable by
#      :func:`datetime.datetime.fromisoformat` in Python 3.10+.

_TZ_SHORT = re.compile(r"([+-])(\d{2})$")


def normalise_timestamp(value: str) -> str | None:
    """Return ``value`` as an ISO-8601 string with a ``T`` separator.

    Args:
        value: Raw timestamp string from a CSV cell.

    Returns:
        Normalised timestamp, or ``None`` if ``value`` is empty.
    """
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    s = s.replace(" ", "T", 1)
    s = _TZ_SHORT.sub(r"\1\2:00", s)
    return s


# ────────────────────────────────────────────────────────────────────────
#  « Numeric parsers »
# ────────────────────────────────────────────────────────────────────────


def parse_float(value: str | None) -> float | None:
    """Parse ``value`` as a float, returning ``None`` for empty cells.

    Args:
        value: Raw CSV cell.

    Returns:
        Float value, or ``None`` for empty / NA strings.
    """
    if value is None:
        return None
    s = value.strip()
    if not s or s in {"NA", "NAN", "nan", "NaN", "null", "NULL"}:
        return None
    return float(s)


def parse_int(value: str | None) -> int | None:
    """Parse ``value`` as an integer (via float to tolerate ``"3.0"``).

    Args:
        value: Raw CSV cell.

    Returns:
        Integer value, or ``None`` for empty / NA strings.
    """
    f = parse_float(value)
    if f is None:
        return None
    return int(f)


# ────────────────────────────────────────────────────────────────────────
#  « Domain-specific cleaners »
# ────────────────────────────────────────────────────────────────────────

# Sentinels used by eShepherd when there is no active virtual fence.
FENCE_DIST_SENTINELS: frozenset[float] = frozenset({-2147483647.0, -2147483648.0})


def parse_fence_distance(value: str | None) -> float | None:
    """Parse a fence-distance cell, stripping eShepherd's no-fence sentinel.

    Args:
        value: Raw CSV cell from a ``Distance_To_Fence_*`` column.

    Returns:
        Parsed float distance, or ``None`` if missing or sentinel.
    """
    f = parse_float(value)
    if f is None:
        return None
    if f in FENCE_DIST_SENTINELS:
        return None
    return f
