"""Tests for :class:`davasus.cosinor.CosinorFitter`."""

from __future__ import annotations

import numpy as np

from davasus.cosinor import CosinorFitter


def _synth_24h(
    acrophase_h: float = 18.0,
    amplitude: float = 0.4,
    mesor: float = 38.6,
    noise_sigma: float = 0.0,
    seed: int = 0,
) -> tuple[np.ndarray, np.ndarray]:
    """Generate a synthetic 24 h cosinor signal with known parameters.

    Uses 24 integer hours — the same input shape the production code
    sees from the SQL hourly aggregation (one row per hour-of-day).
    """
    rng = np.random.default_rng(seed)
    hours = np.arange(24, dtype=float)
    omega = 2.0 * np.pi / 24.0
    values = mesor + amplitude * np.cos(omega * hours - 2 * np.pi * acrophase_h / 24.0)
    if noise_sigma > 0:
        values = values + rng.normal(scale=noise_sigma, size=values.size)
    return hours, values


def test_recovers_known_acrophase_and_amplitude():
    """Closed-form fit recovers a synthetic acrophase to within 0.1 h."""
    h, v = _synth_24h(acrophase_h=18.0, amplitude=0.4)
    fit = CosinorFitter()
    r = fit.fit(h, v)
    assert r.success
    assert abs(r.acrophase_h - 18.0) < 0.1
    assert abs(r.amplitude - 0.4) < 0.02
    assert abs(r.mesor - 38.6) < 0.02


def test_acrophase_is_in_zero_to_period_window():
    """Acrophase wraps into ``[0, 24)`` even for negative phase angles."""
    h, v = _synth_24h(acrophase_h=2.0)
    r = CosinorFitter().fit(h, v)
    assert 0.0 <= r.acrophase_h < 24.0
    assert abs(r.acrophase_h - 2.0) < 0.1


def test_too_few_distinct_hours_returns_failure():
    """Below ``min_hours`` distinct hours, the fit fails cleanly."""
    fit = CosinorFitter(min_hours=10)
    r = fit.fit([0.0, 1.0, 2.0], [1.0, 2.0, 3.0])
    assert r.success is False


def test_predict_evaluates_to_mesor_at_acrophase_plus_quarter_period():
    """``y`` at acrophase + 6 h sits at the mesor for a 24 h cosine."""
    h, v = _synth_24h(acrophase_h=18.0)
    fit = CosinorFitter()
    fit.fit(h, v)
    r = fit.result
    pred = float(fit.predict([r.acrophase_h + 6.0])[0])
    assert abs(pred - r.mesor) < 1e-3
