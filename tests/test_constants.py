"""Tests for :mod:`davasus.constants`."""

from __future__ import annotations

import re

from davasus.constants import (
    COLOURS,
    RCPARAMS,
    THI_REFERENCE,
    WONG_GREY,
    WONG_PALETTE,
    colour_for_category,
    colour_for_group,
    colour_for_variable,
)

_HEX = re.compile(r"^#[0-9A-Fa-f]{6}$")


def test_wong_palette_is_eight_distinct_hex_colours():
    """Wong 2011 palette has eight valid hex strings, all distinct."""
    assert len(WONG_PALETTE) == 8
    for h in WONG_PALETTE:
        assert _HEX.match(h), f"not a 6-digit hex: {h!r}"
    assert len(set(WONG_PALETTE)) == 8


def test_every_colour_value_is_hex():
    """Every leaf in COLOURS is a valid 6-digit hex code."""
    for section in COLOURS.values():
        for value in section.values():
            if isinstance(value, dict):
                for v in value.values():
                    assert _HEX.match(v), f"not hex: {v!r}"
            else:
                assert _HEX.match(value), f"not hex: {value!r}"


def test_variable_lookup_returns_known_colour():
    """`colour_for_variable` resolves a known key."""
    assert colour_for_variable("rumen_temp") == "#D55E00"


def test_variable_lookup_falls_back():
    """Unknown variable keys return the configured default."""
    assert colour_for_variable("nonexistent") == WONG_GREY


def test_category_and_group_lookups():
    """`colour_for_category` and `colour_for_group` work for known keys."""
    assert colour_for_category("fit_line") == "#D55E00"
    assert colour_for_group("sex", "f") == "#CC79A7"
    assert colour_for_group("origin", "DE") == "#009E73"


def test_thi_reference_threshold():
    """The Hoffmann (2020) reference is the documented value."""
    assert THI_REFERENCE == 68.8


def test_rcparams_includes_editable_svg():
    """The rcparams ship the editable-text SVG configuration."""
    assert RCPARAMS["svg.fonttype"] == "none"
    assert RCPARAMS["pdf.fonttype"] == 42
