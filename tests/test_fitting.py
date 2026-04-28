"""Tests for :class:`davasus.fitting.BrokenStickRegressor`."""

from __future__ import annotations

import numpy as np

from davasus.fitting import BrokenStickRegressor


def _synth_two_segment(
    n: int = 600,
    bp: float = 70.0,
    intercept: float = 38.5,
    slope_below: float = 0.0,
    slope_above: float = 0.05,
    noise_sigma: float = 0.05,
    seed: int = 42,
) -> tuple[np.ndarray, np.ndarray]:
    """Generate synthetic broken-stick data with known parameters."""
    rng = np.random.default_rng(seed)
    x = rng.uniform(50.0, 90.0, size=n)
    y_clean = (
        intercept
        + slope_below * x
        + (slope_above - slope_below) * np.maximum(0.0, x - bp)
    )
    y = y_clean + rng.normal(scale=noise_sigma, size=n)
    return x, y


def test_recovers_known_breakpoint():
    """Fit recovers a synthetic breakpoint within 1 THI unit."""
    x, y = _synth_two_segment(bp=70.0)
    fit = BrokenStickRegressor()
    result = fit.fit(x, y)
    assert result.success
    assert abs(result.breakpoint - 70.0) < 1.0
    assert abs(result.slope_below - 0.0) < 0.01
    assert abs(result.slope_above - 0.05) < 0.02
    assert result.r2 > 0.7


def test_predict_matches_fit_at_breakpoint():
    """The two segments meet at the breakpoint."""
    x, y = _synth_two_segment()
    fit = BrokenStickRegressor()
    result = fit.fit(x, y)
    pred_at_bp_minus = fit.predict([result.breakpoint - 1e-6])[0]
    pred_at_bp_plus = fit.predict([result.breakpoint + 1e-6])[0]
    assert abs(pred_at_bp_minus - pred_at_bp_plus) < 1e-3


def test_too_few_points_returns_failure():
    """With < min_points samples the fit returns success=False."""
    fit = BrokenStickRegressor(min_points=50)
    result = fit.fit([1.0, 2.0, 3.0], [10.0, 20.0, 30.0])
    assert result.success is False
    assert "few points" in result.reason


def test_biological_constraint_rejects_inverted_v():
    """If the data turns down past the bp the fit reports failure."""
    rng = np.random.default_rng(7)
    x = rng.uniform(50.0, 90.0, size=400)
    bp_true = 70.0
    y = 39.0 + 0.05 * x + (-0.10 - 0.05) * np.maximum(0.0, x - bp_true)
    y += rng.normal(scale=0.02, size=x.size)
    fit = BrokenStickRegressor()
    result = fit.fit(x, y)
    assert result.success is False
    assert "constraint" in result.reason
