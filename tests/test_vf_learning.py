"""Tests for :mod:`davasus.vf.learning`."""

from __future__ import annotations

import math

import pandas as pd
import pytest

from davasus.vf.learning import (
    compute_block_success_ratios,
    compute_success_ratio,
    convert_audio_pulse_ratio_to_sr,
)


def test_success_ratio_basic():
    assert compute_success_ratio(10, 2) == pytest.approx(0.8)


def test_success_ratio_nan_without_audio():
    assert math.isnan(compute_success_ratio(0, 3))


@pytest.mark.parametrize(("audio", "pulse"), [(10, 2), (4, 4), (7, 0)])
def test_ratio_conversion_matches_counts(audio, pulse):
    ratio = audio / (audio + pulse)
    assert convert_audio_pulse_ratio_to_sr(ratio) == pytest.approx(
        compute_success_ratio(audio, pulse)
    )


@pytest.mark.parametrize("code", [0.0, -1.0])
def test_ratio_codes_are_not_values(code):
    assert math.isnan(convert_audio_pulse_ratio_to_sr(code))


def test_block_sr_sums_counts_before_dividing():
    exposures = pd.DataFrame({"audio": [1, 9, 2, 2, 2], "pulse": [1, 0, 2, 0, 0]})
    blocks = compute_block_success_ratios(exposures, block_size=2)
    assert blocks["n_exposures"].tolist() == [2, 2, 1]
    assert blocks.loc[0, "sr"] == pytest.approx(0.9)
    assert blocks.loc[1, "sr"] == pytest.approx(0.5)
